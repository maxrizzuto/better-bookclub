<script lang="ts">
	import { enhance } from '$app/forms';
	import { flip } from 'svelte/animate';
	import { fly } from 'svelte/transition';

	const users: string[] = $state([]);

	// [TODO] function for add user, export serverside function that checks user (not form action)
	function addUser() {
		const uname = (document.getElementById('uname') as HTMLFormElement).value.trim();
		const errorMessage = document.getElementById('inputError')!;
		errorMessage.textContent = '';
		if (!uname) {
			errorMessage.textContent = 'User input is empty.';
		} else if (users.includes(uname)) {
			errorMessage.textContent = 'User has already been added.';
		} else {
			users.unshift(uname);
		}
		(document.getElementById('uname')! as HTMLFormElement).value = '';
	}

	function removeUser(uname: string) {
		users.splice(users.indexOf(uname), 1);
	}

	function handleEnter(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			e.preventDefault();
			addUser();
		}
	}
</script>

<div id="page-container">
	<div id="title-block">
		<h1 id="title">want ideas for your next book club book?</h1>
	</div>

	<div id="form">
		<form method="POST" name="unamesForm" id="unamesForm" use:enhance>
			<div class="inputs">
				<input
					type="text"
					id="uname"
					placeholder="add StoryGraph usernames here"
					onkeydown={handleEnter}
				/>
				<button type="button" id="addUser" onclick={addUser}>+</button>
				<button type="submit" id="submit">Submit</button>
			</div>
			<div id="inputError"></div>
			<div id="users">
				{#each users as user (user)}
					<div
						class="user"
						id={user}
						animate:flip={{ duration: 400 }}
						transition:fly={{ y: -5, duration: 400 }}
					>
						<!-- [TODO] add validation icon if uname exists -->
						<input type="hidden" name="user" value={user} />
						<button class="removeUser" type="button" onclick={() => removeUser(user)}>-</button>
						<p>{user}</p>
					</div>
				{/each}
			</div>
		</form>
	</div>
</div>

<style>
	/* GLOBALS */
	* {
		font-family: 'Oranienbaum', 'Forum', serif;
		box-sizing: border-box;
	}

	:global(body) {
		min-height: 100vh;
		background: #fcffc7;
		background: linear-gradient(
			137deg,
			rgba(252, 255, 199, 1) 0%,
			rgba(228, 199, 235, 1) 50%,
			rgba(188, 224, 182, 1) 100%
		);
	}

	/* OVERALL PAGE */
	#page-container {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		padding: 0px 15vw;
	}

	/* TITLE */
	#title-block {
		display: flex;
		justify-content: center;
		align-items: flex-end;
		height: 35vh;
		margin-bottom: 4vh;
		margin-top: 5vh;
	}

	#title {
		font-weight: bold;
		font-style: italic;
		font-size: 5rem;
		max-width: 70vw;
		text-align: flex-start;
	}

	/* FORM AND INPUTS */

	#form {
		display: flex;
		justify-content: center;
		align-items: center;
		font-size: 1.25rem;
	}

	#inputError {
		color: red;
		font-size: 0.75em;
		margin: 10px 20px;
		height: 0.75em;
	}

	input {
		width: 50vw;
		height: 75px;
		border-radius: 20px;
		font-size: 1.25em;
		padding-left: 20px;
		max-width: 1000px;
		text-align: left;
		border: 1px black solid;
		font-family: monospace;
	}

	input:focus::placeholder {
		color: transparent;
	}

	#addUser {
		display: inline-block;
		box-sizing: content-box;
		height: 50px;
		width: 50px;
		margin-left: 5px;
		font-size: 2em;
		background-color: white;
		border: 1px lightslategray solid;
		color: darkslategray;
		border-radius: 50%;
		padding: 0px;
		transition: 200ms ease;
	}

	#addUser:hover {
		background-color: lightgray;
	}

	.inputs {
		display: flex;
		align-items: center;
	}

	#submit {
		display: inline-block;
		border-radius: 50%;
		height: 50px;
		width: 50px;
		margin-left: 5px;
		background-color: white;
		border: 1px solid lightslategray;
		transition: 200ms ease-in-out;
	}

	#submit:hover {
		background-color: darkseagreen;
	}

	/* ADDED USERS */

	.removeUser {
		color: white;
		background-color: lightcoral;
		font-size: 2em;
		width: 1em;
		height: 1em;
		margin-right: 10px;
		border-radius: 50%;
		display: flex;
		align-items: center;
		justify-content: center;
		transition: 200ms ease-in-out;
	}

	.removeUser:hover {
		background-color: red;
	}

	#users {
		display: flex;
		padding-left: 20px;
		width: 100%;
		flex-direction: column;
		flex-wrap: wrap;
	}

	.user {
		padding: 10px 5px;
		flex: 1;
		display: flex;
		align-items: center;
		padding: 20px 0px;
		border-bottom: 1px gray solid;
	}

	.user:last-of-type {
		padding-bottom: 0;
		border-bottom: none;
	}
</style>
